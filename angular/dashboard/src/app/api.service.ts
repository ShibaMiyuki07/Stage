import { Injectable } from "@angular/core";

import { HttpClient } from "@angular/common/http";

@Injectable({
    providedIn : "root"
})

export class ApiService{
    constructor(private HttpClient : HttpClient){}


    public getBundle(page : number)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/bundle/'+page)
    }

    public getTopup(page : number)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/topup/'+page)
    }

    public getOm(page : number)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/om/'+page)
    }

    public getUsage(page : number)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/usage/'+page)
    }

    public getEc(page : number)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/ec/'+page)
    }

    public getE_rc(page : number)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/e-rc/'+page)
    }

    public getRoaming(page : number)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/roaming/'+page)
    }

    public getdetails(day :string,type : Int16Array)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/details/'+day+'/'+type)
    }
}

