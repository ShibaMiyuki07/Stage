import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RetraitementManuelComponent } from './retraitement-manuel.component';

describe('RetraitementManuelComponent', () => {
  let component: RetraitementManuelComponent;
  let fixture: ComponentFixture<RetraitementManuelComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [RetraitementManuelComponent]
    });
    fixture = TestBed.createComponent(RetraitementManuelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
