import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { HttpClientModule } from '@angular/common/http';
import { ListComponent } from './list/list.component';
import { DetailsBundleComponent } from './details-bundle/details-bundle.component';
import { DetailsTopupComponent } from './details-topup/details-topup.component';
import { DetailsOmComponent } from './details-om/details-om.component';
import { DetailsEcComponent } from './details-ec/details-ec.component';
import { DetailsRoamingComponent } from './details-roaming/details-roaming.component'

@NgModule({
  declarations: [
    AppComponent,
    ListComponent,
    DetailsBundleComponent,
    DetailsTopupComponent,
    DetailsOmComponent,
    DetailsEcComponent,
    DetailsRoamingComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
