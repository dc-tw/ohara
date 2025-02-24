/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { WORKSPACES } from '../../src/constants/urls';
import * as utils from '../utils';

describe('WorkspacesPage', () => {
  before(() => {
    cy.deleteAllWorkers();
  });

  it('creates a new connect worker cluster', () => {
    const nodeName = Cypress.env('nodeHost');
    const clusterName = utils.makeRandomStr();
    const port = utils.makeRandomPort();

    cy.registerWorker(clusterName);

    cy.visit(WORKSPACES)
      .getByText('New workspace')
      .click()
      .getByPlaceholderText('cluster00')
      .type(clusterName)
      .getByLabelText('Port')
      .click()
      .type(port)
      .getByText('Add node')
      .click();

    cy.get('.ReactModal__Content')
      .eq(1)
      .within(() => {
        cy.getByText(nodeName)
          .click()
          .getByText('Add')
          .click();
      })
      .getByText(nodeName)
      .should('have.length', 1)
      .getByText('Add plugin')
      .click();

    cy.uploadJar(
      'input[type=file]',
      'plugin/ohara-it-sink.jar',
      'ohara-it-sink.jar',
      'application/java-archive',
    ).wait(500);

    cy.get('div.ReactModal__Content')
      .eq(1)
      .within(() => {
        cy.getByText('Add').click();
      });

    cy.get('.ReactModal__Content').should('have.length', 1);
    cy.getByText('ohara-it-sink.jar').should('have.length', 1);
    cy.get('div.ReactModal__Content')
      .eq(0)
      .within(() => {
        cy.getByText('Add').click();
      });

    cy.getByText(clusterName).should('have.length', 1);
  });
});
