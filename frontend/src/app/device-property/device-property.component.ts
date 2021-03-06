import { Component, OnInit, Input } from '@angular/core';
import {DeviceProperty, DeviceService, FeaturePropertyResult} from '../device.service';

@Component({
    selector: 'app-device-property',
    templateUrl: './device-property.component.html',
    styleUrls: ['./device-property.component.scss'],
})
export class DevicePropertyComponent implements OnInit {
    @Input()
    property: DeviceProperty;
    @Input()
    featureIdentifier: string;
    @Input()
    featureOriginator: string;
    @Input()
    featureCategory: string;
    @Input()
    featureVersionMajor: number;
    @Input()
    deviceUUID: string;
    returnValues: FeaturePropertyResult[] = [];
    execute = '';
    expand = false;

    constructor(private deviceService: DeviceService) {}

    ngOnInit(): void {
        this.returnValues = [{
            name: 'test_name',
            value: '[None]',
        }];
    }

    async getProperty(name: string) {
        console.log('testing 1',
            this.returnValues = await this.deviceService.getFeatureProperty(
                this.deviceUUID,
                this.featureOriginator,
                this.featureCategory,
                this.featureIdentifier,
                this.featureVersionMajor,
                name
            )
        );
        console.log(this.returnValues);
        console.log(this.returnValues.find(item => item.name === name.toLowerCase()).value);
    }
}
