import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsRoamingComponent } from './details-roaming.component';

describe('DetailsRoamingComponent', () => {
  let component: DetailsRoamingComponent;
  let fixture: ComponentFixture<DetailsRoamingComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DetailsRoamingComponent]
    });
    fixture = TestBed.createComponent(DetailsRoamingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
