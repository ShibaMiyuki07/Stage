import { Component } from '@angular/core';

@Component({
  selector: 'app-retraitement-manuel',
  templateUrl: './retraitement-manuel.component.html',
  styleUrls: ['./retraitement-manuel.component.css']
})
export class RetraitementManuelComponent {
  usage_type = [
    { id : 1,name : "usage"},
    {id : 2,name : "bundle"},
    {id : 3,name : "topup"},
    {id : 4,name:'om'},
    {id : 5,name : 'ec'},
    {id : 6,name : 'e-rc'},
    {id : 7,name : "roaming"},
    {id : 8,name : "parc"},
    {id : 9,name : "nomad"}
  ];

  date_debut : any = "";
  date_fin : any = "";
  usage : any = "";

  lancer_retraitement()
  {
    window.location.href = "/execution_retraitement?date_debut="+this.date_debut+"&date_fin="+this.date_fin+"&usage_type="+this.usage
  }
}
