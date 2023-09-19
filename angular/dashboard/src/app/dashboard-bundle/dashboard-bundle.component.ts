import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import * as Chart from 'chart.js';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-dashboard-bundle',
  templateUrl: './dashboard-bundle.component.html',
  styleUrls: ['./dashboard-bundle.component.css']
})
export class DashboardBundleComponent implements OnInit{
  liste : any;
  data_bundle_amnt : any;
  data_bundle_cnt : any;
  label : any;
  title : any;
  type : any;
  erreur : any = "";
  constructor(private ApiService : ApiService,private route : ActivatedRoute){}
  ngOnInit(): void {
    this.route.queryParams.subscribe(params =>{
      this.type = params['type'];
      this.ApiService.getDashboard("","",params['type']).toPromise().then((data) => {
        this.liste = data;
        this.data_bundle_amnt = this.liste.data.map((donne : any) => donne.bndle_amnt);
        this.data_bundle_cnt = this.liste.data.map((donne : any) => donne.bndle_cnt)
        this.title = this.liste.data.map((donne: any) =>donne.usage_type);
        this.label = this.liste.data.map((donne: any) =>new Date(donne.day).toLocaleDateString());
        var test = new Chart('bundle_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu des offres",
              data : this.data_bundle_amnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
  
        var test2 = new Chart('bundle_cnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Nombre d'offre",
              data : this.data_bundle_cnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
      }).catch(error =>
        {
          this.erreur = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page";
        })
    })
    
  }

}
