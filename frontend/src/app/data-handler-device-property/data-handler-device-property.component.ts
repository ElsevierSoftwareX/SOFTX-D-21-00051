import {Component, Input, OnInit, SimpleChanges} from '@angular/core';
import { DeviceProperty } from '../device.service';
import {CheckboxParam, DatabaseService} from '../database.service';
// import {CustomPollingInterval} from '../data-handler-device-feature/data-handler-device-feature.component';

export interface CustomPollingInterval {
    duration: number;
}

@Component({
  selector: 'app-data-handler-device-property',
  templateUrl: './data-handler-device-property.component.html',
  styleUrls: ['./data-handler-device-property.component.scss']
})
export class DataHandlerDevicePropertyComponent implements OnInit {
    @Input()
    property: DeviceProperty;
    @Input()
    featureId: number;
    @Input()
    featureIdentifier: string;
    @Input()
    featureOriginator: string;
    @Input()
    featureCategory: string;
    @Input()
    featureVersionMajor: number;
    @Input()
    uuid: string;
    execute = '';
    expand = false;
    checkboxes: CheckboxParam[] = [];
    dataTransferCheckbox = false;  // Delete once properly implemented
    metaDataCheckbox = false;  // Delete once properly implemented
    defaultPollingInterval = 60;
    customPollingInterval: CustomPollingInterval;

    constructor(private databaseService: DatabaseService) {}

    async setCheckboxCommandLevel(uuid: string, featureId: number, commandId: number, meta: boolean,
                                  active: boolean, metaInterval: number, nonMetaInterval: number) {
        console.log(meta, active, metaInterval, nonMetaInterval);
        // Check parameters
        if (meta === undefined || null) { meta = false; }
        if (active === undefined || null) { active = false; }

        await this.databaseService.setCheckboxPropertyLevel(uuid, featureId, commandId,
            meta, active, metaInterval, nonMetaInterval);
        // Maybe I need to refresh here,but hopefully the two-way binding works...
        // Maybe I should just pass the commandInterface
    }
    // ngOnChanges(changes: SimpleChanges) {
    // Update and send changes of checkboxes to backend
    //    }
    ngOnInit(): void {
    }

}
