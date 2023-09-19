import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DashboardBundleComponent } from './dashboard-bundle.component';

describe('DashboardBundleComponent', () => {
  let component: DashboardBundleComponent;
  let fixture: ComponentFixture<DashboardBundleComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DashboardBundleComponent]
    });
    fixture = TestBed.createComponent(DashboardBundleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
