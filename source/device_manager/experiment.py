from dataclasses import dataclass
from typing import List
from datetime import datetime
from enum import IntEnum
from uuid import UUID
import msgpack
import aioredis
from source.device_manager.database import get_database_connection, get_redis_pool, release_database_connection
from source.device_manager.scheduler import BookingInfo, BookingInfoWithNames, get_device_bookings_for_experiment_inside_transaction, book_inside_transaction


class ExperimentStatus(IntEnum):
    WAITING_FOR_EXECUTION = 0
    SUBMITED_FOR_EXECUTION = 1
    RUNNING = 2
    FINISHED_SUCCESSFUL = 3
    FINISHED_ERROR = 4
    FINISHED_MANUALLY = 5
    UNKNOWN = 6


@dataclass
class SchedulingInfo:
    id: int
    name: str
    start: int
    end: int
    scriptID: int


@dataclass
class Experiment:
    id: int
    name: str
    start: int
    end: int
    user: str
    deviceBookings: List[BookingInfoWithNames]
    scriptID: int
    scriptName: int


def get_experiment_name(id: int) -> str:
    conn = get_database_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('select name from experiments where id=%s', [id])
        return cursor.fetchone()
    except:
        raise
    finally:
        cursor.close()
        release_database_connection(conn)


def get_experiment_user(id: int) -> int:
    conn = get_database_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('select userID from experiments where id=%s', [id])
        return cursor.fetchone()
    except:
        raise
    finally:
        cursor.close()
        release_database_connection(conn)


def get_experiment(id: int) -> Experiment:
    experiment=None
    conn = get_database_connection() 
    with conn:
        with conn.cursor() as cursor:
            cursor.execute('select experiments.id, experiments.name,experiments.startTime,'\
                           'experiments.endTime,users.name,experiments.script,scripts.name '\
                           'from experiments join scripts on experiments.script=scripts.id '\
                           'join users on experiments.userID=users.id '\
                           'where experiments.id=%s',[id])
            result = cursor.fetchone()
            experiment = Experiment(result[0], result[1], result[2], result[3],
                                    result[4], [], result[5], result[6])

            experiment.deviceBookings = get_device_bookings_for_experiment_inside_transaction(
                conn, experiment.id)
    release_database_connection(conn)
    return experiment


def get_all_experiments() -> List[Experiment]:
    experiments=[]
    conn = get_database_connection() 
    with conn:
        with conn.cursor() as cursor:
            cursor.execute('select experiments.id, experiments.name,experiments.startTime,'\
                           'experiments.endTime,users.name,experiments.script,scripts.name '\
                           'from experiments join scripts on experiments.script=scripts.id '\
                           'join users on experiments.userID=users.id')
            experiments = [
                Experiment(row[0], row[1], row[2], row[3], row[4], [], row[5],
                           row[6]) for row in cursor
            ]

            for experiment in experiments:
                experiment.deviceBookings = get_device_bookings_for_experiment_inside_transaction(
                    conn, experiment.id)
    release_database_connection(conn)
    return experiments


def get_user_experiments(user: int) -> List[Experiment]:
    experiments=[]
    conn = get_database_connection() 
    with conn:
        with conn.cursor() as cursor:
            cursor.execute('select experiments.id, experiments.name,experiments.startTime,'\
                           'experiments.endTime,experiments.userID,experiments.script,scripts.name '\
                           'from experiments join scripts on experiments.script=scripts.id where experiments.userID=%s',[user])
            experiments = [
                Experiment(row[0], row[1], row[2], row[3], row[4], [], row[5],
                           row[6]) for row in cursor
            ]

            for experiment in experiments:
                experiment.deviceBookings = get_device_bookings_for_experiment_inside_transaction(
                    conn, experiment.id)
    release_database_connection(conn)
    return experiments


def get_scheduling_info() -> List[SchedulingInfo]:
    now = int(datetime.timestamp(datetime.now()))
    info = []
    conn = get_database_connection() 
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                'select id, name, startTime, endTime, script '
                'from experiments '
                'where experiments.startTime>=%s '
                'order by experiments.startTime', [now])
            info = [
                SchedulingInfo(row[0], row[1], row[2], row[3], row[4])
                for row in cursor
            ]
    release_database_connection(conn)
    return info
    

def create_experiment(name: str, start: int, end: int, user: int,
                      devices: List[UUID], script: int) -> int:
    print(name, start, end, user, devices, script)
    experiment_id = -1
    conn = get_database_connection() 
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                'insert into experiments values (default,%s,%s,%s,%s,%s) returning id',
                [name, start, end, user, script])
            experiment_id = cursor.fetchone()[0]
            for device in devices:
                if book_inside_transaction(
                        conn,
                        BookingInfo(-1, name, start, end, user, device,
                                    experiment_id)) < 0:
                    conn.rollback()
                    experiment_id = -1
                    break
    release_database_connection(conn)
    return experiment_id


def edit_experiment(experiment_id: int, name: str, start: int, end: int, user: int,
                    devices: List[UUID], script: int) -> int:
    print(experiment_id, name, start, end, user, devices, script)
    conn = get_database_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(
                'update experiments set name=%s, startTime=%s, endTime=%s, userID=%s, script=%s where id=%s',
                [name, start, end, user, script, experiment_id])
            # Delete existing bookings for experiment
            cursor.execute(
                'delete from bookings where experiment=%s',
                [experiment_id])
            # Create new bookings
            for device in devices:
                if book_inside_transaction(
                        conn,
                        BookingInfo(-1, name, start, end, user, device,
                                    experiment_id)) < 0:
                    # If a device cannot be booked in the new time frame, rollback all changes
                    conn.rollback()
                    experiment_id = -1
                    break
    release_database_connection(conn)
    return experiment_id


def delete_experiment(experiment_id: int):
    conn = get_database_connection() 
    with conn:
        with conn.cursor() as cursor:
            cursor.execute('delete from experiments where id=%s',
                           [experiment_id])
            cursor.execute('delete from bookings where experiment=%s',
                           [experiment_id])

    release_database_connection(conn)


async def _publish_command(command: str, params: list):
    pool = await get_redis_pool()
    with await pool as conn:
        await conn.publish(
            'scheduler', msgpack.packb({
                'command': command,
                'params': params
            }))


async def receive_experiment_status(channel):
    # while await channel.wait_message():
    return msgpack.unpackb(await channel.get(), raw=False)


async def start_experiment(experiment_id: int):
    await _publish_command('start', [experiment_id])


async def stop_experiment(experiment_id: int):
    await _publish_command('stop', [experiment_id])


async def get_status(experiment_id: int):
    await _publish_command('status', [experiment_id])
