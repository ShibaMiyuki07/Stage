import { Component, NgIterable, OnInit } from '@angular/core';
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
  constructor (private apiservice : ApiService){}
  ngOnInit() {
    this.apiservice.getBundle().toPromise().then((data : any) => this.bundlelist = data) 
    this.apiservice.getTopup().toPromise().then((data : any) => this.topuplist = data) 
    this.apiservice.getOm().toPromise().then((data : any) => this.omlist = data) 
    this.apiservice.getUsage().toPromise().then((data : any) => this.usagelist = data) 
    this.apiservice.getEc().toPromise().then((data : any) => this.eclist = data) 
    this.apiservice.getE_rc().toPromise().then((data : any) => this.e_rclist = data) 
  }
}
