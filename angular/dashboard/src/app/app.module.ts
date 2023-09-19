import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { HttpClientModule } from '@angular/common/http';
import { ListComponent } from './list/list.component';
import { DetailsBundleComponent } from './details-bundle/details-bundle.component';
import { DashboardBundleComponent } from './dashboard-bundle/dashboard-bundle.component';
import { DashboardEcComponent } from './dashboard-ec/dashboard-ec.component';
import { DashboardOmComponent } from './dashboard-om/dashboard-om.component';
import { DashboardRoamingComponent } from './dashboard-roaming/dashboard-roaming.component';
import { DashboardTopupComponent } from './dashboard-topup/dashboard-topup.component';
import { DashboardUsageComponent } from './dashboard-usage/dashboard-usage.component';
import { RetraitementComponent } from './retraitement/retraitement.component';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { VerificationManuelComponent } from './verification-manuel/verification-manuel.component';
import { ExecuteVerificationComponent } from './execute-verification/execute-verification.component';
import { RetraitementManuelComponent } from './retraitement-manuel/retraitement-manuel.component';
import { ExecutionRetraitementComponent } from './execution-retraitement/execution-retraitement.component';
import { LoginComponent } from './login/login.component';
import {NgbModule} from '@ng-bootstrap/ng-bootstrap';  


@NgModule({
  declarations: [
    AppComponent,
    ListComponent,
    DetailsBundleComponent,
    DashboardBundleComponent,
    DashboardEcComponent,
    DashboardOmComponent,
    DashboardRoamingComponent,
    DashboardTopupComponent,
    DashboardUsageComponent,
    RetraitementComponent,
    VerificationManuelComponent,
    ExecuteVerificationComponent,
    RetraitementManuelComponent,
    ExecutionRetraitementComponent,
    LoginComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,
    ReactiveFormsModule,
    FormsModule,
    NgbModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule {

 }
