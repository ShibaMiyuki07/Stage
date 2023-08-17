import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-details-usage',
  templateUrl: './details-usage.component.html',
  styleUrls: ['./details-usage.component.css']
})
export class DetailsUsageComponent implements OnInit{
  usagedetails : any;
  constructor(private ApiService : ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => this.ApiService.getUsage_details(params['day']).toPromise().then((data) => this.usagedetails = data))
  }

}
