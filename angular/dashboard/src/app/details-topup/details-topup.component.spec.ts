import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsTopupComponent } from './details-topup.component';

describe('DetailsTopupComponent', () => {
  let component: DetailsTopupComponent;
  let fixture: ComponentFixture<DetailsTopupComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DetailsTopupComponent]
    });
    fixture = TestBed.createComponent(DetailsTopupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
