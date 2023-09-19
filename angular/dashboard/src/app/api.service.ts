import { Injectable } from "@angular/core";

import { HttpClient } from "@angular/common/http";
import { Utilisateur } from "./utilisateur";

@Injectable({
    providedIn : "root"
})



export class ApiService{
    link = "http://127.0.0.1:5000";
    constructor(private HttpClient : HttpClient){}


    public getListe(type : number,page : number)
    {
        return this.HttpClient.get(this.link+'/liste/'+type+'/'+page)
    }

    public getdetails(day :string,type : Int16Array)
    {
        return this.HttpClient.get(this.link+'/details/'+day+'/'+type)
    }

    public getDashboard(date_debut : string,date_fin : string,type : number)
    {
        if (date_debut != "" && date_fin != ""){
            return this.HttpClient.get(this.link+'/dashboard/'+type+'/'+date_debut+'/'+date_fin)
        }
        return this.HttpClient.get(this.link+'/dashboard/'+type)
    }

    public retraitement(day : string,type : number)
    {
        return this.HttpClient.get(this.link+'/retraitement/'+day+'/'+type)
    }

    public fichier_log(day : string,type : number)
    {
        return this.HttpClient.get(this.link+'/fichier_log/'+day+'/'+type,{ responseType: 'text' })
    }

    public fichier_log_retraitement(date_debut : string,date_fin : string,type : number)
    {
        return this.HttpClient.get(this.link+"/log_retraitement/"+date_debut+"/"+date_fin+"/"+type,{ responseType: 'text' });
    }

    public verification(date_debut:string,date_fin : string,type : number)
    {
        return this.HttpClient.get(this.link+"/verification/"+date_debut+"/"+date_fin+"/"+type);
    }

    public authentification(user : Utilisateur)
    {
        return this.HttpClient.post<any>(this.link+"/login",user);
    }
}

