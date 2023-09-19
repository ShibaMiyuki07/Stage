import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { ListComponent } from './list/list.component';
import { DetailsBundleComponent } from './details-bundle/details-bundle.component';
import { DashboardBundleComponent } from './dashboard-bundle/dashboard-bundle.component';
import { DashboardEcComponent } from './dashboard-ec/dashboard-ec.component';
import { DashboardOmComponent } from './dashboard-om/dashboard-om.component';
import { DashboardRoamingComponent } from './dashboard-roaming/dashboard-roaming.component';
import { DashboardTopupComponent } from './dashboard-topup/dashboard-topup.component';
import { DashboardUsageComponent } from './dashboard-usage/dashboard-usage.component';
import { RetraitementComponent } from './retraitement/retraitement.component';
import { VerificationManuelComponent } from './verification-manuel/verification-manuel.component';
import { ExecuteVerificationComponent } from './execute-verification/execute-verification.component';
import { RetraitementManuelComponent } from './retraitement-manuel/retraitement-manuel.component';
import { ExecutionRetraitementComponent } from './execution-retraitement/execution-retraitement.component';
import { AuthGuard } from './guards/auth.guard';
import { LoginComponent } from './login/login.component';


const routes: Routes = [
{
  path : 'liste_retraitement',component : ListComponent,canActivate : [AuthGuard]
},
{
  path : 'details',component : DetailsBundleComponent,canActivate : [AuthGuard]
},{
  path : 'dashboard_bundle',component : DashboardBundleComponent,canActivate : [AuthGuard]
},{
  path : 'dashboard_ec',component : DashboardEcComponent,canActivate : [AuthGuard]
},{
  path : 'dashboard_om',component : DashboardOmComponent,canActivate : [AuthGuard]
},{
  path : 'dashboard_roaming',component : DashboardRoamingComponent,canActivate : [AuthGuard]
},{
  path : 'dashboard_topup',component : DashboardTopupComponent,canActivate : [AuthGuard]
},{
  path : 'dashboard_usage',component : DashboardUsageComponent,canActivate : [AuthGuard]
},{
  path : 'retraitement',component : RetraitementComponent,canActivate : [AuthGuard]
},{
  path : 'verification_manuel',component : VerificationManuelComponent,canActivate : [AuthGuard]
},{
  path : 'execution_verification',component : ExecuteVerificationComponent,canActivate : [AuthGuard]
},{
  path : 'retraitement_manuel',component : RetraitementManuelComponent,canActivate : [AuthGuard]
},{
  path : 'execution_retraitement',component : ExecutionRetraitementComponent,canActivate : [AuthGuard]
},{
  path : 'login',component : LoginComponent
},
{ path :"",redirectTo:"login",pathMatch:"full"}];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
