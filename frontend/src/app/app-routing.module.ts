import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { LoginComponent } from './login/login.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { AuthGuard } from './auth.guard';


const routes: Routes = [
    {
        path: '',
        component: DashboardComponent,
        canActivate: [AuthGuard],
    },
    {
        path: 'login',
        component: LoginComponent,
        canActivate: [AuthGuard],
    },
    // {
    //     path: 'calendar',
    //     component: CalendarComponent,
    //     canActivate: [AuthGuard],
    // },
    // {
    //     path: 'experiments',
    //     component: ExperimentsComponent,
    //     canActivate: [AuthGuard],
    // },
    // {
    //     path: 'scripts',
    //     component: ScriptsComponent,
    //     canActivate: [AuthGuard],
    // },
    // {
    //     path: 'dataHandler',
    //     component: DataHandlerComponent,
    //     canActivate: [AuthGuard],
    // },
    // {
    //     path: 'log',
    //     component: LogViewComponent,
    //     canActivate: [AuthGuard],
    // },
    // {
    //     path: 'about',
    //     component: AboutComponent},
    // {
    //     path: 'adminArea',
    //     component: AdminAreaComponent,
    //     canActivate: [AuthGuard],
    // },
    // {
    //     path: 'userArea',
    //     component: UserAreaComponent,
    //     canActivate: [AuthGuard],
    // },
];

@NgModule({
    imports: [RouterModule.forRoot(routes, { relativeLinkResolution: 'legacy' })],
    exports: [RouterModule],
})
export class AppRoutingModule {}
