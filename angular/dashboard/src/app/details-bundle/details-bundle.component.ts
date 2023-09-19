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
  type : any;
  day : any;
  message : any = {"erreur" : ""};
  constructor(private ApiService:ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    setTimeout(() =>{
      this.route.queryParams.subscribe(params => {this.type = params['type'];this.day = params['day']})
      this.ApiService.getdetails(this.day,this.type).subscribe(data => {this.bundleDetails = data},error => {this.message['erreur'] = "Impossible de prendre les données"});
    },100)
  }
}
