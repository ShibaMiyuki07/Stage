import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import * as Chart from 'chart.js';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-dashboard-roaming',
  templateUrl: './dashboard-roaming.component.html',
  styleUrls: ['./dashboard-roaming.component.css']
})
export class DashboardRoamingComponent {
  liste : any;
  data_sms_o_amnt : any;
  data_voice_o_amnt : any;
  data_data_amnt : any;
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
        this.data_sms_o_amnt = this.liste.data.map((donne : any) => donne.sms_o_amnt);
        this.data_voice_o_amnt = this.liste.data.map((donne : any) => donne.voice_o_amnt)
        this.data_data_amnt = this.liste.data.map((donne : any) => donne.data_amnt)
        this.title = this.liste.data.map((donne: any) =>donne.usage_type);
        this.label = this.liste.data.map((donne: any) =>new Date(donne.day).toLocaleDateString());
        var test = new Chart('sms_o_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu sms sortant roaming",
              data : this.data_sms_o_amnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
  
        var test2 = new Chart('voice_o_amnt_chart',{
          type: 'line',
          data: {
            labels: this.label,
            datasets: [{
              label: "Revenu voix sortant roaming",
              data : this.data_voice_o_amnt,
              borderColor : 'black'
            }]
          },
          options: {
            responsive: true
        }
        });
  
        
      }).catch(error => {
        this.erreur = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page"
      })
    })
    
  }
}
