import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { ListComponent } from './list/list.component';
import { DetailsBundleComponent } from './details-bundle/details-bundle.component';


const routes: Routes = [{
  path : 'liste_retraitement',component : ListComponent
},
{
  path : 'details',component : DetailsBundleComponent
}];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
