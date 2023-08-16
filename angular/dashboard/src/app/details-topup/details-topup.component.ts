import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-details-topup',
  templateUrl: './details-topup.component.html',
  styleUrls: ['./details-topup.component.css']
})
export class DetailsTopupComponent implements OnInit{
  topupdetails : any;
  constructor(private ApiService:ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => this.ApiService.getTopup_details(params['day']).toPromise().then((data) => this.topupdetails = data))
  }

}
