import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardOmComponent } from './dashboard-om.component';

describe('DashboardOmComponent', () => {
  let component: DashboardOmComponent;
  let fixture: ComponentFixture<DashboardOmComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DashboardOmComponent]
    });
    fixture = TestBed.createComponent(DashboardOmComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
