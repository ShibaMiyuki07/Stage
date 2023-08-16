import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsOmComponent } from './details-om.component';

describe('DetailsOmComponent', () => {
  let component: DetailsOmComponent;
  let fixture: ComponentFixture<DetailsOmComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DetailsOmComponent]
    });
    fixture = TestBed.createComponent(DetailsOmComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
