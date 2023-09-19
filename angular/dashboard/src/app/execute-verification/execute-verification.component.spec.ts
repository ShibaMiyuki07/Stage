import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ExecuteVerificationComponent } from './execute-verification.component';

describe('ExecuteVerificationComponent', () => {
  let component: ExecuteVerificationComponent;
  let fixture: ComponentFixture<ExecuteVerificationComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ExecuteVerificationComponent]
    });
    fixture = TestBed.createComponent(ExecuteVerificationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
