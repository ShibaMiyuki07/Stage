import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-details-bundle',
  templateUrl: './details-bundle.component.html',
  styleUrls: ['./details-bundle.component.css']
})
export class DetailsBundleComponent implements OnInit{
  bundleDetails : any;
  constructor(private ApiService:ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => this.ApiService.getdetails(params['day'],params['type']).toPromise().then((data) => this.bundleDetails = data))
  }

}
