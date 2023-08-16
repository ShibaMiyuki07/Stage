import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-details-ec',
  templateUrl: './details-ec.component.html',
  styleUrls: ['./details-ec.component.css']
})
export class DetailsEcComponent implements OnInit{
  ecdetails : any;
  constructor(private ApiService : ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => this.ApiService.getEc_details(params['day']).toPromise().then((data) => this.ecdetails = data))
  }
}