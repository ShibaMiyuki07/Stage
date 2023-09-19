import { Component, OnInit } from '@angular/core';
import { ApiService } from '../api.service';
import { Utilisateur } from '../utilisateur';
import { Router } from '@angular/router';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent implements OnInit {
  username :string ="";
  password : string ="";
  error : string = ""
  user :  Utilisateur = {username : this.username,password : this.password};
  constructor(private ApiService : ApiService,private route : Router){}
    ngOnInit(): void {

    }

  login()
  {
    this.user.username = this.username;
    this.user.password = this.password;
    this.ApiService.authentification(this.user).toPromise().then(data =>{
      if(data == 0)
      {
        this.error = "Identifiant inexistant"
      }
      else
      {
        localStorage.setItem('isLoggedIn',"true");
        window.location.href='/retraitement_manuel'
      }
    })
  }
}
