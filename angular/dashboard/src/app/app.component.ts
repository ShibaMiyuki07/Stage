import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent{

  constructor(private route : Router){}
  title = 'dashboard';
  log = localStorage.getItem('isLoggedIn')

  deconnexion()
  {
    localStorage.removeItem('isLoggedIn');
    this.route.navigate(['/login']);
  }
}
