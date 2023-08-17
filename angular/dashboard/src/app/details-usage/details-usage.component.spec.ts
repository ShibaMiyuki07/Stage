import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsUsageComponent } from './details-usage.component';

describe('DetailsUsageComponent', () => {
  let component: DetailsUsageComponent;
  let fixture: ComponentFixture<DetailsUsageComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DetailsUsageComponent]
    });
    fixture = TestBed.createComponent(DetailsUsageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
