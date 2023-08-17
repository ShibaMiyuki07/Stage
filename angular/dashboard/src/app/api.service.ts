import { Injectable } from "@angular/core";

import { HttpClient } from "@angular/common/http";

@Injectable({
    providedIn : "root"
})



export class ApiService{
    link = "http://127.0.0.1:8000";
    constructor(private HttpClient : HttpClient){}


    public getBundle(page : number)
    {
        return this.HttpClient.get(this.link+'/bundle/'+page)
    }

    public getTopup(page : number)
    {
        return this.HttpClient.get(this.link+'/topup/'+page)
    }

    public getOm(page : number)
    {
        return this.HttpClient.get(this.link+'/om/'+page)
    }

    public getUsage(page : number)
    {
        return this.HttpClient.get(this.link+'/usage/'+page)
    }

    public getEc(page : number)
    {
        return this.HttpClient.get(this.link+'/ec/'+page)
    }

    public getE_rc(page : number)
    {
        return this.HttpClient.get(this.link+'/e-rc/'+page)
    }

    public getRoaming(page : number)
    {
        return this.HttpClient.get(this.link+'/roaming/'+page)
    }

    public getdetails(day :string,type : Int16Array)
    {
        return this.HttpClient.get(this.link+'/details/'+day+'/'+type)
    }
}

