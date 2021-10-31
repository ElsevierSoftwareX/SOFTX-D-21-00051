import queue
import time
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from threading import Thread
from typing import Optional
from uuid import UUID

from app.schemas import Job
from app.util import docker_helper
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler import events

from util.database import get_scheduling_info


class ProcessStatusEventType(IntEnum):
    STARTED = 0
    FINISHED_SUCCESSFUL = 1
    FINISHED_MANUALLY = 2
    ERROR = 3


@dataclass
class ProcessStatusEvent:
    job_uuid: UUID
    event_type: ProcessStatusEventType
    message: Optional[str] = None


class JobStatus(IntEnum):
    WAITING_FOR_EXECUTION = 0
    SUBMITTED_FOR_EXECUTION = 1
    RUNNING = 2
    FINISHED_SUCCESSFUL = 3
    FINISHED_ERROR = 4
    FINISHED_MANUALLY = 5
    UNKNOWN = 6


@dataclass
class JobState:
    job_uuid: UUID
    title: str
    scheduled_job_id: int
    container_id: str
    start_time: datetime
    # end_time: int
    status: JobStatus
    # logs: deque


event_queue = queue.SimpleQueue()
process_status_queue = queue.SimpleQueue()
scheduler = BackgroundScheduler()

jobs = {}
scheduled_jobs = {}


def event_listener(event):
    event_queue.put(event)


def change_job_status(job_uuid: UUID, status: JobStatus):
    jobs[job_uuid].status = status
    # redis_connection.publish(
    #     'experiment_status',
    #     msgpack.packb({
    #         'experimentId': experiment_id,
    #         'status': status
    #     }))


def start_job(job: Job, status_queue: queue.SimpleQueue):
    # devices = [
    #     asdict(get_device_info(booking.device)) for booking in exp.deviceBookings
    # ]
    # start_data_handling_for_experiment(exp)
    container = docker_helper.create_flow_container(job.flow)
    print(f'Created docker container for job {job.uuid}: \"{container.name}\"')

    # output_thread = Thread(target=print_container_output, args=(container, job.uuid, ), daemon=True)
    wait_thread = Thread(target=wait_until_container_stops,
                         args=(container, job.uuid, status_queue))
    # Starting threads
    # container.start()
    # output_thread.start()
    wait_thread.start()

    status_queue.put(
        ProcessStatusEvent(job.uuid, ProcessStatusEventType.STARTED,
                           container.id))
    return


def wait_until_container_stops(container, job_uuid: UUID,
                               status_queue: queue.SimpleQueue):
    status = container.wait()
    # print(container.logs())
    print(f'container stopped with StatusCode {status["StatusCode"]}')
    if status['StatusCode'] == 0:
        status_queue.put(
            ProcessStatusEvent(job_uuid,
                               ProcessStatusEventType.FINISHED_SUCCESSFUL))
    else:
        status_queue.put(
            ProcessStatusEvent(job_uuid, ProcessStatusEventType.ERROR))
    container.remove()


# def print_container_output(container, experiment_id):
#     # container_output = container.attach(logs=False, stream=True)
#     container_output = container.logs(follow=True, timestamps=True, stream=True, stdout=True, stderr=True)
#     print('Output thread started!')
#     log_path = os.path.join(TEMP_DIRECTORY, 'container', f'{datetime.now().strftime("%d_%m_%Y-%H_%M_%S")}_{str(experiment_id)}.log')
#     with open(log_path, "w") as file:
#         for line in container_output:
#             print(line.decode())
#             file.write(line.decode())
#             # json.dump(line.decode(), file)
#             forward_experiment_log(experiment_id=experiment_id, logging_message=line.decode())


def handle_process_status_events(event: ProcessStatusEvent):
    if event.event_type == ProcessStatusEventType.STARTED:
        jobs[event.job_uuid].container_id = event.message
        change_job_status(event.job_uuid, JobStatus.RUNNING)
        print(f'{jobs[event.job_uuid].name} started')
    elif event.event_type == ProcessStatusEventType.FINISHED_SUCCESSFUL:
        change_job_status(event.job_uuid,
                          JobStatus.FINISHED_SUCCESSFUL)
        print(f'{jobs[event.job_uuid].name} finished successful')
    elif event.event_type == ProcessStatusEventType.ERROR:
        change_job_status(event.job_uuid,
                          JobStatus.FINISHED_ERROR)
        print(f'{jobs[event.job_uuid].name} Error')


def handle_scheduling_jobs(event):
    if event.code == events.EVENT_JOB_SUBMITTED:
        if event.job_id in scheduled_jobs:
            job_uuid = scheduled_jobs[event.job_id]
            job_entry = jobs[job_uuid]
            change_job_status(job_uuid, JobStatus.SUBMITTED_FOR_EXECUTION)
            print(f'{job_entry.title} submitted')
    elif event.code == events.EVENT_JOB_REMOVED:
        if event.job_id in scheduled_jobs:
            job_entry = jobs[scheduled_jobs[event.job_id]]
            print(f'{job_entry.title} removed')
    elif event.code == events.EVENT_JOB_ERROR:
        if event.job_id in scheduled_jobs:
            job_uuid = scheduled_jobs[event.job_id]
            job_entry = jobs[job_uuid]
            change_job_status(job_uuid,
                              JobStatus.FINISHED_ERROR)
            print(f'scheduling error for {job_entry.title}')
    elif event.code == events.EVENT_JOB_MISSED:
        if event.job_id in scheduled_jobs:
            job_uuid = scheduled_jobs[event.job_id]
            job_entry = jobs[job_uuid]
            change_job_status(job_uuid,
                              JobStatus.FINISHED_ERROR)
            print(f'job {job_entry.title} missed')


def schedule_future_jobs_from_database():
    for job in get_scheduling_info():
        schedule_job(job)


def schedule_job(job: Job):
    if (job.uuid not in jobs) or (
            jobs[job.uuid].status == JobStatus.FINISHED_ERROR or
            jobs[job.uuid].status == JobStatus.FINISHED_SUCCESSFUL
            or jobs[job.uuid].status
            == JobStatus.FINISHED_MANUALLY):
        scheduled_job = scheduler.add_job(start_job,
                                          'date',
                                          args=[job, process_status_queue],
                                          name=f'job: {job.title}',
                                          run_date=job.execute_at)
        jobs[job.uuid] = JobState(
            job.uuid, job.title, scheduled_job.id, '0', job.created_at, JobStatus.WAITING_FOR_EXECUTION)
        scheduled_jobs[scheduled_job.id] = job.uuid
        # change_experiment_status(job.id,
        #                          ExperimentStatus.WAITING_FOR_EXECUTION)


def main():
    scheduler.start()
    schedule_future_jobs_from_database()
    scheduler.add_listener(event_listener, events.EVENT_ALL)

    t = time.time()
    while True:
        try:
            handle_scheduling_jobs(event_queue.get_nowait())
        except queue.Empty:
            pass

        try:
            handle_process_status_events(process_status_queue.get_nowait())
        except queue.Empty:
            pass

        # receive_and_execute_commands()
        t2 = time.time()
        if t2 - t >= 5:
            t = t2
            schedule_future_jobs_from_database()

    scheduler.shutdown()


if __name__ == '__main__':
    main()