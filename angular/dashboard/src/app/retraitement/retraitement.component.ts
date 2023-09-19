import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'app-retraitement',
  templateUrl: './retraitement.component.html',
  styleUrls: ['./retraitement.component.css']
})
export class RetraitementComponent implements OnInit{
  log : any;
  test = 0;
  type : any;
  day :any;
  interval : any;
  data : any;
  message : any = "";
  error : any;
  constructor(private apiservice : ApiService,private route : ActivatedRoute){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => {this.type = params['type'];this.day = params['day']})
    this.interval = setInterval(()=>{
      this.apiservice.fichier_log(this.day,this.type).subscribe(data => this.log = data,error =>{
        clearInterval(this.interval);
        this.error = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page";
      });
    },3000);
    /*this.apiservice.retraitement(this.day,this.type).subscribe((data) =>{
      clearInterval(this.interval);
      this.message = data
    },
    error =>{
      this.error = "Veuillez vérifier les ports de notre webservice"
    })*/
  }



  confirmation_quitter()
  {
    if(confirm("Le retraitement continuera mmême si vous quitter mais il vous est impossible de revoir le log.\nSouhaitez vous continuer quand même?"))
    {
      window.location.href="/liste_retraitement?type="+this.type;
    }
  }
  
}
