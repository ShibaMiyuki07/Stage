import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardEcComponent } from './dashboard-ec.component';

describe('DashboardEcComponent', () => {
  let component: DashboardEcComponent;
  let fixture: ComponentFixture<DashboardEcComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DashboardEcComponent]
    });
    fixture = TestBed.createComponent(DashboardEcComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
