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

    public getTopup()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/topup')
    }

    public getOm()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/om')
    }

    public getUsage()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/usage')
    }

    public getEc()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/usage')
    }
}

