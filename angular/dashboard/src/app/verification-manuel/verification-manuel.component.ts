import { Component } from '@angular/core';

@Component({
  selector: 'app-verification-manuel',
  templateUrl: './verification-manuel.component.html',
  styleUrls: ['./verification-manuel.component.css']
})
export class VerificationManuelComponent {
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

  lancer_verification()
  {
    window.location.href = "/execution_verification?date_debut="+this.date_debut+"&date_fin="+this.date_fin+"&usage_type="+this.usage
  }
}
