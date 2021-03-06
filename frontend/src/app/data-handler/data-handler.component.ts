import {Component, OnInit, ViewChild} from '@angular/core';
import {
    animate,
    state,
    style,
    transition,
    trigger,
} from '@angular/animations';
import {DatabaseService, Database, DatabaseStatus} from '../database.service';
import { ActivatedRoute } from '@angular/router';
import {element} from 'protractor';
import {MatDialogRef} from '@angular/material/dialog';
import { MatTable } from '@angular/material/table';
import { MatDialog } from '@angular/material/dialog';
import {
    DeviceService,
    Device,
    DeviceStatus,
} from '../device.service';
import {DatabaseLinkComponent} from '../database-link/database-link.component';
import {AddDatabaseComponent} from '../add-database/add-database.component';

interface RowDataDevice {
    device: Device;
    status: DeviceStatus;
    database?: Database;
    databaseStatus?: DatabaseStatus;
    detailsLoaded: boolean;

}
interface RowDataDatabase {
    database: Database;
    status: DatabaseStatus;
    detailsLoaded: boolean;
}

@Component({
  selector: 'app-data-handler',
  templateUrl: './data-handler.component.html',
  styleUrls: ['./data-handler.component.scss'],
  animations: [
    trigger('detailExpand', [
        state('collapsed', style({ height: '0px', minHeight: '0' })),
        state('expanded', style({ height: '*' })),
        transition(
            'expanded <=> collapsed',
            animate('225ms cubic-bezier(0.4, 0.0, 0.2, 1)')
        ),
    ]),
    ],
})
export class DataHandlerComponent implements OnInit {
    dataSource: RowDataDevice[] = [];
    databasesSource: RowDataDatabase[] = [];
    tableDeviceColumns = [
        'name',
        'database',
        'address',
        'port',
        'online',
        'policy',
        'edit',
        'data_transfer',
    ];
    tableDatabaseColumns = [
        'name',
        'username',
        'address',
        'port',
        'online',
        'status',
        'edit',
    ];
    dataTransferCheckbox = false;
    metaDataCheckbox = false;
    selected: number | null = null;
    @ViewChild(MatTable) tableDevices: MatTable<any>;
    @ViewChild(MatTable) tableDatabases: MatTable<any>;

    databases = [];
    newDatabase: Database = {id: 1234567890, name: 'InfluxDB', address: '127.0.0.1', port: 8888, username: 'root', password: 'root'};
    database: Database;
    selectedDatabase: Database;

  constructor(private route: ActivatedRoute,
              private databaseService: DatabaseService,
              public deviceService: DeviceService,
              public dialog: MatDialog
              ) {
        this.newDatabase = {
            id: 1234567890,
            name: 'InfluxDB',
            address: '127.0.0.1',
            port: 8888,
            username: 'root',
            password: 'root',
      };
  }
  async addDatabase() {
        const dialogRef = this.dialog.open(AddDatabaseComponent
        );
        const result = await dialogRef.afterClosed().toPromise();
        console.log(result);
        // await this.databaseService.setDatabase('123456789', result); // This is the real implementation
        await this.databaseService.addDatabase(result);  // This is a mockup implementation
        window.alert(`Database ${result.name} added`);
        await this.refreshDatabases();
  }
  async deleteDatabase(database) {
        // var _database: Database;
        // this.selectedDatabase = this.databases.find(element => element.name === database.name);
        // console.log(this.selectedDatabase);
        await this.databaseService.deleteDatabase(database.id);
        await this.refreshDatabases();
  }

  async getDevices() {
        const deviceList = await this.deviceService.getDeviceList();
        console.log('Returning devices');
        console.log(deviceList);
        const data: RowDataDevice[] = [];
        if (this.dataSource.length === 0) {
            console.log('Im empty inside');
        } else {

            console.log('Ive got friends:', this.dataSource.length);
        }

        for (const dev of deviceList) {
            let db: Database = {
                name: '-',
                address: '-',
                port: 0,
                username: '-',
                password: '-',
            };
            // If available, get the info of the linked database
            if (dev.databaseId !== null && dev.databaseId !== undefined) {
                db = await this.databaseService.getDatabase(dev.databaseId);
            }
            data.push({
                device: dev,
                status: {online: false, status: ''},
                database: db ,
                databaseStatus: {online: false, status: ''},
                detailsLoaded: false,
            });
        }
        this.dataSource = data;
        for (let i = 0; i < this.dataSource.length; i++) {
            // Get the status of the device itself
            const promise = this.deviceService.getDeviceStatus(
                this.dataSource[i].device.uuid
            );
            promise.then((status) => {
                console.log(status);
                this.dataSource[i].status = status;
            });
            // Get the status of the linked database
            const databaseId = this.dataSource[i].device.databaseId;
            if (databaseId !== null && databaseId !== undefined) {
                const promiseDB = this.databaseService.getDatabaseStatus(
                    this.dataSource[i].device.databaseId
                );
                promiseDB.then((status) => {
                    this.dataSource[i].databaseStatus = status;
                    this.tableDevices.renderRows();
                });
            }
        }
        this.tableDevices.renderRows();
    }

  async getDatabases() {
        const databaseList = await this.databaseService.getDatabases();
        const databaseData: RowDataDatabase[] = [];
        for (const db of databaseList) {
            databaseData.push({
                database: db,
                status: {online: false, status: ''},
                detailsLoaded: false,
            });
        }
        this.databasesSource = databaseData;
        this.tableDatabases.renderRows();
        for (let i = 0; i < this.databasesSource.length; i++) {
            const promise = this.databaseService.getDatabaseStatus(
                this.databasesSource[i].database.id
            );
            await promise.then((status) => {
                this.databasesSource[i].status = status;
                this.tableDevices.renderRows();
            });
        }
  }

  async link(i: number) {
        const dialogRef = this.dialog.open(DatabaseLinkComponent, {
            data: this.dataSource[i].device,
        });
        const result = await dialogRef.afterClosed().toPromise();
        if (result.databaseId === 0) {
            await this.databaseService.deleteDatabaseLinkToDevice(this.dataSource[i].device.uuid);
        }
        else {
            await this.databaseService.linkDatabaseToDevice(this.dataSource[i].device.uuid, result.databaseId);
        }
        await this.refreshDatabases();
        await this.refreshDevices();
  }
  async refreshDevices() {
        await this.getDevices();
  }
  async refreshDatabases() {
        await this.getDatabases();
  }
  showDetails(i: number) {
        this.selected = this.selected === i ? null : i;
        this.dataSource[i].detailsLoaded = true;
  }

  async setCheckboxDeviceLevel(device: Device, active: boolean) {
      await this.databaseService.setCheckboxDeviceLevel(device.uuid, active);
      this.refreshDevices();
  }

  ngOnInit(): void {
        // this.databases = this.databaseService.getDatabases();
        this.route.paramMap.subscribe(params => {
        this.getDevices();
        this.getDatabases();
      });
  }

}
