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

import React from 'react';
import { cleanup, render } from 'react-testing-library';
import 'jest-dom/extend-expect';

import WorkspacesNewModal from '../WorkspacesNewModal';

jest.mock('api/workerApi');
jest.mock('api/brokerApi');
jest.mock('api/zookeeperApi');

const props = {
  isActive: true,
  onClose: jest.fn(),
  onConfirm: jest.fn(),
};

afterEach(cleanup);

describe('<WorkspacesNewModal />', () => {
  it('renders the page', () => {
    render(<WorkspacesNewModal {...props} />);
  });
});
