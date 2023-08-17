import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-details-roaming',
  templateUrl: './details-roaming.component.html',
  styleUrls: ['./details-roaming.component.css']
})
export class DetailsRoamingComponent implements OnInit{
  roamingdetails : any;
  constructor(private ApiService : ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => this.ApiService.getRoaming_details(params['day']).toPromise().then((data) => this.roamingdetails = data))
  }

}
