import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardUsageComponent } from './dashboard-usage.component';

describe('DashboardUsageComponent', () => {
  let component: DashboardUsageComponent;
  let fixture: ComponentFixture<DashboardUsageComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DashboardUsageComponent]
    });
    fixture = TestBed.createComponent(DashboardUsageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
