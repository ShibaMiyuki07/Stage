import { ComponentFixture, TestBed } from '@angular/core/testing';

import { VerificationManuelComponent } from './verification-manuel.component';

describe('VerificationManuelComponent', () => {
  let component: VerificationManuelComponent;
  let fixture: ComponentFixture<VerificationManuelComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [VerificationManuelComponent]
    });
    fixture = TestBed.createComponent(VerificationManuelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
