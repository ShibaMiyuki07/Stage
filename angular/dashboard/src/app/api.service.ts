import { Injectable } from "@angular/core";

import { HttpClient } from "@angular/common/http";

@Injectable({
    providedIn : "root"
})

export class ApiService{
    constructor(private HttpClient : HttpClient){}


    /**
     * Liste Bundle et details
     */
    public getBundle()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/bundle')
    }


    /**
     * Liste des topups et details
     */
    public getTopup()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/topup')
    }


    /**
     * Liste orange money et details
     */
    public getOm()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/om')
    }


    /**
     * Liste usage et details
     */
    public getUsage()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/usage')
    }

    public getEc()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/ec')
    }

    public getE_rc()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/e-rc')
    }

    public getRoaming()
    {
        return this.HttpClient.get('http://127.0.0.1:8000/roaming')
    }

    public getdetails(day :string,type : Int16Array)
    {
        return this.HttpClient.get('http://127.0.0.1:8000/details/'+day+'/'+type)
    }
}

