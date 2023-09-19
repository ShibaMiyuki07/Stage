import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardTopupComponent } from './dashboard-topup.component';

describe('DashboardTopupComponent', () => {
  let component: DashboardTopupComponent;
  let fixture: ComponentFixture<DashboardTopupComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DashboardTopupComponent]
    });
    fixture = TestBed.createComponent(DashboardTopupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
