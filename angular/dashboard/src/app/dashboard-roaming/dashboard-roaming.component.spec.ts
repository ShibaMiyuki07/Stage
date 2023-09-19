import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardRoamingComponent } from './dashboard-roaming.component';

describe('DashboardRoamingComponent', () => {
  let component: DashboardRoamingComponent;
  let fixture: ComponentFixture<DashboardRoamingComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DashboardRoamingComponent]
    });
    fixture = TestBed.createComponent(DashboardRoamingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
