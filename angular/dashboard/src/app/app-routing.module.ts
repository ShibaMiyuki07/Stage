import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { ListComponent } from './list/list.component';
import { DetailsBundleComponent } from './details-bundle/details-bundle.component';
import { DetailsTopupComponent } from './details-topup/details-topup.component';
import { DetailsOmComponent } from './details-om/details-om.component';
import { DetailsEcComponent } from './details-ec/details-ec.component';
import { DetailsRoamingComponent } from './details-roaming/details-roaming.component';
import { DetailsUsageComponent } from './details-usage/details-usage.component';


const routes: Routes = [{
  path : 'liste_retraitement',component : ListComponent
},
{
  path : 'details_bundle',component : DetailsBundleComponent
},
{
  path : 'details_topup',component : DetailsTopupComponent
},
{
  path : 'details_om',component : DetailsOmComponent
},
{
  path : 'details_ec',component : DetailsEcComponent
},
{
  path : 'details_roaming',component : DetailsRoamingComponent
},
{
  path : 'details_usage',component : DetailsUsageComponent
}];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
