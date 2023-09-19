import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import * as Chart from 'chart.js';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-dashboard-topup',
  templateUrl: './dashboard-topup.component.html',
  styleUrls: ['./dashboard-topup.component.css']
})
export class DashboardTopupComponent {
  liste : any;
  data_rec_cnt : any;
  data_rec_amnt : any;
  label : any;
  title : any;
  type : any;
  erreur : any="";
  constructor(private ApiService : ApiService,private route : ActivatedRoute){}
  ngOnInit(): void {
    this.route.queryParams.subscribe(params =>{
      this.type = params['type'];
      this.ApiService.getDashboard("","",params['type']).toPromise().then((data) => {
        this.liste = data;
        this.data_rec_cnt = this.liste.data.map((donne : any) => donne.rec_cnt);
        this.data_rec_amnt = this.liste.data.map((donne : any) => donne.rec_amnt)
        this.title = this.liste.data.map((donne: any) =>donne.usage_type);
        this.label = this.liste.data.map((donne: any) =>new Date(donne.day).toLocaleDateString());
        var test = new Chart('rec_cnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Nombre de rechargement",
              data : this.data_rec_cnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
  
        var test2 = new Chart('rec_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu des rechargements",
              data : this.data_rec_amnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
      }).catch(error =>{
        this.erreur = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page"
      })
    })
    
  }
}
