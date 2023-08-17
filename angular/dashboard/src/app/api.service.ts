import { Injectable } from "@angular/core";

import { HttpClient } from "@angular/common/http";

@Injectable({
    providedIn : "root"
})

export class ApiService{
    constructor(private HttpClient : HttpClient){}


    public getBundle()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/bundle')
    }

    public getBundle_Details(day : string)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/bundle_details/'+day)
    }

    public getTopup()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/topup')
    }

    public getTopup_details(day:string)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/topup_details/'+day)
    }

    public getOm()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/om')
    }

    public getOm_details(day :string)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/om_details/'+day)
    }

    public getUsage()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/usage')
    }

    public getEc()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/ec')
    }

    public getEc_details(day : string)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/ec_details/'+day)
    }

    public getE_rc()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/e-rc')
    }

    public getRoaming()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/roaming')
    }

    public getRoaming_details(day :string)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/roaming_details/'+day)
    }
}

