import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ApiService } from '../api.service';
import * as Chart from 'chart.js';

@Component({
  selector: 'app-dashboard-om',
  templateUrl: './dashboard-om.component.html',
  styleUrls: ['./dashboard-om.component.css']
})
export class DashboardOmComponent implements OnInit {
  liste : any;
  data_om_amnt : any;
  data_om_tr_amnt : any;
  data_om_cnt : any;
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
        this.data_om_amnt = this.liste.data.map((donne : any) => donne.om_amnt);
        this.data_om_tr_amnt = this.liste.data.map((donne : any) => donne.om_tr_amnt)
        this.data_om_cnt = this.liste.data.map((donne : any) => donne.om_cnt)
        this.title = this.liste.data.map((donne: any) =>donne.usage_type);
        this.label = this.liste.data.map((donne: any) =>new Date(donne.day).toLocaleDateString());
        var test = new Chart('om_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu Orange Money",
              data : this.data_om_amnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
  
        var test2 = new Chart('om_tr_anmt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Somme transaction Orange Money",
              data : this.data_om_tr_amnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
  
        var test3 = new Chart('om_cnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Nombre de transaction Orange Money",
              data : this.data_om_cnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
      }).catch(error => {
        this.erreur = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page";
      })
    })
    
  }
}
