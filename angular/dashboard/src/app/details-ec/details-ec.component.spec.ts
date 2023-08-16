import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsEcComponent } from './details-ec.component';

describe('DetailsEcComponent', () => {
  let component: DetailsEcComponent;
  let fixture: ComponentFixture<DetailsEcComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DetailsEcComponent]
    });
    fixture = TestBed.createComponent(DetailsEcComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
