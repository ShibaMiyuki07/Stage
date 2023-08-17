import { Component, NgIterable, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-list',
  templateUrl: './list.component.html',
  styleUrls: ['./list.component.css']
})
export class ListComponent implements OnInit{
  bundlelist: any[] | undefined;
  topuplist: any[] | undefined;
  omlist : any[] | undefined;
  usagelist : any[] |undefined;
  eclist : any[] | undefined;
  e_rclist : any[] | undefined;
  roaminglist : any[] | undefined;

  bundle_page = 0;
  topup_page = 0;
  om_page = 0;
  usage_page = 0;
  ec_page = 0;
  e_rc_page = 0;
  roaming_page = 0; 
  constructor (private apiservice : ApiService){}
  ngOnInit() {
    this.apiservice.getBundle(this.bundle_page).toPromise().then((data : any) => this.bundlelist = data) 
    this.apiservice.getTopup(this.topup_page).toPromise().then((data : any) => this.topuplist = data) 
    this.apiservice.getOm(this.om_page).toPromise().then((data : any) => this.omlist = data) 
    this.apiservice.getUsage(this.usage_page).toPromise().then((data : any) => this.usagelist = data) 
    this.apiservice.getEc(this.ec_page).toPromise().then((data : any) => this.eclist = data) 
    this.apiservice.getE_rc(this.e_rc_page).toPromise().then((data : any) => this.e_rclist = data) 
    this.apiservice.getRoaming(this.roaming_page).toPromise().then((data : any) => this.roaminglist = data) 
  }

   bundle(nbr : number) {
    if (nbr == 1)
    {
      this.bundle_page ++
    }
    else
    {
      this.bundle_page--
    }
    this.apiservice.getBundle(this.bundle_page).toPromise().then((data : any) => this.bundlelist = data) 
  }
  topup(nbr : number) {
    if (nbr == 1)
    {
      this.topup_page ++
    }
    else
    {
      this.topup_page--
    }
    this.apiservice.getTopup(this.topup_page).toPromise().then((data : any) => this.topuplist = data)  
  }

  om(nbr : number) {
    if (nbr == 1)
    {
      this.om_page ++
    }
    else
    {
      this.om_page--
    }
    this.apiservice.getOm(this.om_page).toPromise().then((data : any) => this.omlist = data) 
  }
  usage(nbr : number) {
    if (nbr == 1)
    {
      this.usage_page ++
    }
    else
    {
      this.usage_page--
    }
    this.apiservice.getUsage(this.usage_page).toPromise().then((data : any) => this.usagelist = data) 
  }

  ec(nbr : number) {
    if (nbr == 1)
    {
      this.ec_page ++
    }
    else
    {
      this.ec_page--
    }
    this.apiservice.getEc(this.ec_page).toPromise().then((data : any) => this.eclist = data)  
  }

  e_rc(nbr : number) {
    if (nbr == 1)
    {
      this.e_rc_page ++
    }
    else
    {
      this.e_rc_page--
    }
    this.apiservice.getE_rc(this.e_rc_page).toPromise().then((data : any) => this.e_rclist = data)  
  }

  roaming(nbr : number) {
    if (nbr == 1)
    {
      this.roaming_page ++
    }
    else
    {
      this.roaming_page--
    }
    this.apiservice.getRoaming(this.roaming_page).toPromise().then((data : any) => this.roaminglist = data) 
  }
}
