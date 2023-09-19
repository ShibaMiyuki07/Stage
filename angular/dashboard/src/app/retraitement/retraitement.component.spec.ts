import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RetraitementComponent } from './retraitement.component';

describe('RetraitementComponent', () => {
  let component: RetraitementComponent;
  let fixture: ComponentFixture<RetraitementComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [RetraitementComponent]
    });
    fixture = TestBed.createComponent(RetraitementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
