import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ExecutionRetraitementComponent } from './execution-retraitement.component';

describe('ExecutionRetraitementComponent', () => {
  let component: ExecutionRetraitementComponent;
  let fixture: ComponentFixture<ExecutionRetraitementComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ExecutionRetraitementComponent]
    });
    fixture = TestBed.createComponent(ExecutionRetraitementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
