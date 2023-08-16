import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-details-om',
  templateUrl: './details-om.component.html',
  styleUrls: ['./details-om.component.css']
})
export class DetailsOmComponent implements OnInit{
  omdetails : any;
  constructor(private ApiService : ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => this.ApiService.getOm_details(params['day']).toPromise().then((data) => this.omdetails = data))
  }
}
