import { Component, OnInit,Inject,
  LOCALE_ID } from '@angular/core';
import { ApiService } from '../api.service';
import { ActivatedRoute } from '@angular/router';
import {
  formatDate
 }
  from '@angular/common';
  import { NgbModal } from '@ng-bootstrap/ng-bootstrap';



@Component({
  selector: 'app-list',
  templateUrl: './list.component.html',
  styleUrls: ['./list.component.css']
})
export class ListComponent implements OnInit{
  liste: any;
  type = 0;
  page = 1;
  erreur : any = "";
  closeResult : string = "";
  log : any;
  interval : any;
  erreur_log : string = "";
  constructor ( @Inject(LOCALE_ID) public locale: string,private apiservice : ApiService,private route : ActivatedRoute,private modalService:NgbModal){}
  ngOnInit() {
    this.route.queryParams.subscribe(params => this.type = params['type'])
    this.apiservice.getListe(this.type,this.page).toPromise().then((data : any) => this.liste = data).catch(error =>{
      setTimeout(() => {
        this.erreur = "Impossible de se connecter. Essayer de relancer le web service et rafraichissez la page";
      },1000)
    })
  }

  change_page(page_number : number)
  {
    if(page_number == 1)
    {
      this.page++
    }
    if(page_number == -1)
    {
      this.page--
    }
    setTimeout(()=>{
      this.apiservice.getListe(this.type,this.page).toPromise().then((data : any) => this.liste = data)
    },100)
  }

  check_next()
  {
    if((this.page)>this.liste.nbr_doc/7 || this.page == this.liste.nbr_doc/7)
    {
      return 0
    }
    return 1
  }

  open(content : any,date : any,type : number) {  
    date = formatDate(date,'YYYY-MM-dd',this.locale);
    this.apiservice.fichier_log(date,type).subscribe(data => {
      this.log = data;
      this.erreur_log = "";
      this.interval = setInterval(()=>{
        this.apiservice.fichier_log(date,type).subscribe(data => this.log = data,error => {
          this.erreur_log = "Le log n'existe pas";
          clearInterval(this.interval);
        })
      },3000);
    },error => {
      this.erreur_log = "Le log n'existe pas";
    });
    
    this.modalService.open(content, {ariaLabelledBy: 'modal-basic-title'}).result.then((result) => {  
      this.closeResult = `Closed with: ${result}`; 
    }, (reason) => {  
      this.closeResult = `Dismissed`;  
    });  
  }  

}
