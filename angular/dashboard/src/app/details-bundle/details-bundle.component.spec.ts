import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsBundleComponent } from './details-bundle.component';

describe('DetailsBundleComponent', () => {
  let component: DetailsBundleComponent;
  let fixture: ComponentFixture<DetailsBundleComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [DetailsBundleComponent]
    });
    fixture = TestBed.createComponent(DetailsBundleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
