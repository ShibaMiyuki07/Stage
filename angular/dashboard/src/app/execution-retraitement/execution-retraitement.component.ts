import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ApiService } from '../api.service';

@Component({
  selector: 'app-execution-retraitement',
  templateUrl: './execution-retraitement.component.html',
  styleUrls: ['./execution-retraitement.component.css']
})
export class ExecutionRetraitementComponent implements OnInit {
  constructor(private ApiService : ApiService,private route:ActivatedRoute){}
  type : any;
  date_debut : any;
  date_fin : any;
  log : any;
  interval : any;
  interval_message : any;
  message : any = {"log" : "","erreur":""};
  titre : any;
  veuillez_patienter : any ="Veuillez patienter"
  ngOnInit(): void {
    this.route.queryParams.subscribe(params => {
      this.type = params['usage_type'];
      this.date_debut = params['date_debut'];
      this.date_fin = params['date_fin']});
      if(this.type == "" || (this.date_debut =="" && this.date_fin ==""))
      {
        this.message['error'] = "Erreur de donné envoyé"
        this.log = "Veuillez choisir des données valides"
      }
      else
      { 
        let i =0;
        this.interval = setInterval(() =>{
          if(i != 3)
          {
            this.veuillez_patienter+=".";
            i++;
          }
          else
          {
            this.veuillez_patienter = this.veuillez_patienter.replaceAll('.','');
            i=0;
          }
          
        },1000);
        this.interval_message = setInterval(()=>{
          this.ApiService.fichier_log_retraitement(this.date_debut,this.date_fin,this.type).subscribe(data =>{
            this.log = data;
          })
        },1000);
        if(this.date_debut == "" && this.date_fin != "")
        {
          this.date_debut = this.date_fin;
        }
        if(this.date_fin == "" && this.date_debut !="")
        {
          this.date_fin = this.date_debut;
        }
        this.titre = "Retraitement du "+this.date_debut+" à "+this.date_fin
        this.ApiService.verification(this.date_debut,this.date_fin,this.type).subscribe(data => {
          clearInterval(this.interval_message);
          clearInterval(this.interval);
          this.veuillez_patienter = "Execution terminé";
          this.message = data
        },error =>{
          setTimeout(() =>{
            clearInterval(this.interval);
            clearInterval(this.interval_message);
            this.veuillez_patienter = "Execution terminé";
            this.titre = "Retraitement du "+this.date_debut+" à "+this.date_fin;
            this.message['error'] = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page";
          },1)
        });
      }
    
  } 
  confirmation_quitter()
  {
    if(this.message['log'] != "" || this.message != "" )
    {
      window.location.href="/retraitement_manuel";
    }
    if(this.message["log"] == "" && this.message['error'] == "")
    {
      if(confirm("Le retraitement continuera mmême si vous quitter mais il vous est impossible de revoir le log.\nSouhaitez vous continuer quand même?"))
      {
        window.location.href="/retraitement_manuel";
      }
    }
  }
}
