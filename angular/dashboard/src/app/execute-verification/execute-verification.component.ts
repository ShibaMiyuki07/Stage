import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-execute-verification',
  templateUrl: './execute-verification.component.html',
  styleUrls: ['./execute-verification.component.css']
})
export class ExecuteVerificationComponent implements OnInit {
  constructor(private ApiService : ApiService,private route:ActivatedRoute){}
  type : any;
  date_debut : any;
  date_fin : any;
  data : any;
  log : any;
  message : any = {"log" : "","error":""};
  veuillez_patienter : string = "Veuillez patienter";
  interval :any;
  execution_en_cours : string = "En cours d'execution";
  verification : string = "";
  ngOnInit(): void {
    this.route.queryParams.subscribe(params => {
      this.type = params['usage_type'];
      this.date_debut = params['date_debut'];
      this.date_fin = params['date_fin']
    }
      );
      if(this.type === "" || (this.date_debut =="" && this.date_fin == "") )
      {
        this.message['error'] = "Veuillez choisir des données valides"
        this.veuillez_patienter = ""
        this.execution_en_cours = "Execution termine"
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
        if(this.date_fin == "" && this.date_debut !="")
        {
          this.date_fin = this.date_debut;
        }
        else if(this.date_debut == "" && this.date_fin != "")
        {
          this.date_debut = this.date_fin
        }

        if(this.date_fin > this.date_debut)
        {
          this.verification = "Verification du "+this.date_debut+" à "+this.date_fin
        }
        else
        {
          this.verification = "Verification du "+this.date_fin+" à "+this.date_debut
        }
        this.ApiService.verification(this.date_debut,this.date_fin,this.type).subscribe(data => {
          clearInterval(this.interval);
          this.veuillez_patienter = "";
          this.execution_en_cours = "Execution terminé"
          this.message = data;
        },error =>{
          setTimeout(() =>{
            clearInterval(this.interval);
            this.veuillez_patienter = "";
            this.execution_en_cours = "Execution terminé";
            this.message['error'] = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page";
          },5000)
        });
      }
    
  }

  confirmation_quitter()
  {
    if((this.message['error'] != "" || this.message['log'] != ''))
    {
      window.location.href="/verification_manuel";
    }
    else
    {
      if(confirm("La vérification en cours continuera même si vous quittez cette page. \nContinuer quand même?"))
      {
        window.location.href="/verification_manuel";
      }
    }
  }

}
