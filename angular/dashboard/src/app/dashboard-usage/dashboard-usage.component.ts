import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import * as Chart from 'chart.js';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-dashboard-usage',
  templateUrl: './dashboard-usage.component.html',
  styleUrls: ['./dashboard-usage.component.css']
})
export class DashboardUsageComponent {
  liste : any;
  data_sms_o_amnt : any;
  data_voice_o_amnt : any;
  data_data_amnt : any;
  data_sms_vas_amnt : any;
  data_voice_vas_amnt : any;
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
        this.data_sms_o_amnt = this.liste.data.map((donne : any) => donne.sms_o_amnt);
        this.data_voice_o_amnt = this.liste.data.map((donne : any) => donne.voice_o_amnt)
        this.data_data_amnt = this.liste.data.map((donne : any) => donne.data_amnt)
        this.data_sms_vas_amnt = this.liste.data.map((donne : any) => donne.sms_vas_amnt)
        this.data_voice_vas_amnt = this.liste.data.map((donne : any) => donne.voice_vas_amnt)
        this.title = this.liste.data.map((donne: any) =>donne.usage_type);
        this.label = this.liste.data.map((donne: any) =>new Date(donne.day).toLocaleDateString());
        var test = new Chart('voice_o_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu voix sortant usage global",
              data : this.data_voice_o_amnt,
              borderColor : 'black'
            }]
          },
        });
  
        var test2 = new Chart('sms_o_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu sms sortant usage global",
              data : this.data_sms_o_amnt,
              borderColor : 'black'
            }]
          },
        });
        var test3 = new Chart('data_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu data sortant usage global",
              data : this.data_data_amnt,
              borderColor : 'black'
            }]
          },
        });
  
        var test4 = new Chart('voice_vas_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu voix sortant vas usage global",
              data : this.data_voice_vas_amnt,
              borderColor : 'black'
            }]
          },
        });
  
        var test5 = new Chart('sms_vas_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu sms sortant vas usage global",
              data : this.data_sms_vas_amnt,
              borderColor : 'black'
            }]
          },
        });
        
      }).catch(error => {
        this.erreur = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page"
      })
    })
    
  }
}
