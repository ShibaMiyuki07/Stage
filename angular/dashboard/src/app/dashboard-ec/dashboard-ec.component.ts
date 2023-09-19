import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import * as Chart from 'chart.js';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-dashboard-ec',
  templateUrl: './dashboard-ec.component.html',
  styleUrls: ['./dashboard-ec.component.css']
})
export class DashboardEcComponent implements OnInit{
  liste : any;
  data_ec_fees : any;
  data_ec_qty : any;
  label : any;
  title : any;
  type : any;
  erreur : any ="";
  constructor(private ApiService : ApiService,private route : ActivatedRoute){}
  ngOnInit(): void {
    this.route.queryParams.subscribe(params =>{
      this.type = params['type'];
      this.ApiService.getDashboard("","",params['type']).toPromise().then((data) => {
        this.liste = data;
        this.data_ec_fees = this.liste.data.map((donne : any) => donne.ec_loan);
        this.data_ec_qty = this.liste.data.map((donne : any) => donne.ec_qty)
        this.title = this.liste.data.map((donne: any) =>donne.usage_type);
        this.label = this.liste.data.map((donne: any) =>new Date(donne.day).toLocaleDateString());
        var test = new Chart('ec_fees_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu des Lany Credit",
              data : this.data_ec_fees,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
  
        var test2 = new Chart('ec_qty_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Nombre de Lany Credit",
              data : this.data_ec_qty,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
      }).catch(error =>{
        this.erreur = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page";
      })
    })
    
  }
}
