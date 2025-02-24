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
import styled from 'styled-components';
import PropTypes from 'prop-types';
import { NavLink } from 'react-router-dom';
import { get } from 'lodash';

import * as URLS from 'constants/urls';
import { ListLoader } from 'components/common/Loader';
import { fetchInfo } from 'api/infoApi';
import { InfoModal } from '../Modal';

const StyledHeader = styled.div`
  background-color: ${props => props.theme.white};
  position: fixed;
  left: 0;
  top: 0;
  right: 0;
  height: 59px;
  border-bottom: 1px solid ${props => props.theme.lighterGray};
  padding: 0 50px;
  z-index: 100;
`;

StyledHeader.displayName = 'StyledHeader';

const HeaderWrapper = styled.header`
  width: 100%;
  height: 100%;
  max-width: 1200px;
  display: flex;
  align-items: center;
  margin: auto;
`;

HeaderWrapper.displayName = 'Header';

const Brand = styled(NavLink)`
  font-family: Merriweather, sans-serif;
  color: ${props => props.theme.blue};
  font-size: 24px;
  padding: 0;
  display: block;
`;

Brand.displayName = 'Brand';

const Nav = styled.nav`
  margin-left: 54px;
  background-color: ${props => props.theme.white};
`;

Nav.displayName = 'Nav';

const Link = styled(NavLink)`
  color: ${props => props.theme.dimBlue};
  font-size: 14px;
  padding: 15px 0;
  margin: 10px 20px;
  position: relative;
  transition: 0.3s all;

  &:hover,
  &.active {
    color: ${props => props.theme.blue};
  }
`;

Link.displayName = 'Link';

const Btn = styled.button`
  border: none;
  color: ${props => props.theme.dimBlue};
  font-size: 18px;
  background-color: transparent;

  &:hover,
  &.active {
    color: ${props => props.theme.blue};
  }
`;

const Login = styled(Link)`
  margin: 0 10px;
`;

Login.displayName = 'Login';

const Icon = styled.i`
  margin-right: 8px;
`;

Icon.displayName = 'Icon';

const RightCol = styled.div`
  margin-left: auto;
`;

const Ul = styled.ul`
  padding: 22px 25px;

  li {
    margin-bottom: 15px;
    display: flex;
    align-items: center;

    &:last-child {
      margin-bottom: 0;
    }
  }

  .item {
    margin-right: 10px;
    padding: 13px 15px;
    color: ${props => props.theme.darkerBlue};
    background-color: ${props => props.theme.whiteSmoke};
  }

  .content {
    color: ${props => props.theme.lightBlue};
  }

  .item,
  .content {
    font-size: 13px;
  }
`;

const LoaderWrapper = styled.div`
  margin: 30px;
`;

class Header extends React.Component {
  static propTypes = {
    isLogin: PropTypes.bool.isRequired,
    versionInfo: PropTypes.object,
  };

  state = {
    isConfigModalActive: false,
    isVersionModalActive: false,
    partitions: '',
    replicationFactor: '',
    isLoading: true,
  };

  componentDidMount() {
    this.fetchInfo();
  }

  fetchInfo = async () => {
    const res = await fetchInfo();
    this.setState({ isLoading: false });
    const result = get(res, 'data.result', null);

    if (result) {
      this.setState({ info: result });
    }
  };

  handleConfigModalOpen = () => {
    this.setState({ isConfigModalActive: true });
  };

  handleConfigModalClose = () => {
    this.setState({ isConfigModalActive: false });
  };

  handleVersionModalOpen = () => {
    this.setState({ isVersionModalActive: true });
  };

  handleVersionModalClose = () => {
    this.setState({ isVersionModalActive: false });
  };

  render() {
    const { isLogin } = this.props;
    const { isVersionModalActive, info = {}, isLoading } = this.state;

    const { versionInfo = {}, mode = '' } = info;
    const { version, revision, date } = versionInfo;

    return (
      <StyledHeader>
        <InfoModal
          title="Ohara version"
          isActive={isVersionModalActive}
          contentLabel="VersionModal"
          ariaHideApp={false}
          width="440px"
          onRequestClose={this.handleVersionModalClose}
          handleCancel={this.handleVersionModalClose}
          testId="info-modal"
        >
          {isLoading ? (
            <LoaderWrapper>
              <ListLoader />
            </LoaderWrapper>
          ) : (
            <Ul data-testid="info-list">
              <li>
                <span className="item">Mode:</span>
                <span className="content">{mode}</span>
              </li>
              <li>
                <span className="item">Version:</span>
                <span className="content">{version}</span>
              </li>
              <li>
                <span className="item">Revision:</span>
                <span className="content">{revision}</span>
              </li>
              <li>
                <span className="item">Build date:</span>
                <span className="content">{date}</span>
              </li>
            </Ul>
          )}
        </InfoModal>
        <HeaderWrapper>
          <Brand to={URLS.HOME}>Ohara Stream</Brand>
          <Nav>
            <Link
              activeClassName="active"
              data-testid="pipelines-link"
              to={URLS.PIPELINES}
            >
              <Icon className="fas fa-code-branch" />
              <span>Pipelines</span>
            </Link>

            <Link
              activeClassName="active"
              data-testid="nodes-link"
              to={URLS.NODES}
            >
              <Icon className="fas fa-sitemap" />
              <span>Nodes</span>
            </Link>

            <Link
              activeClassName="active"
              data-testid="workspaces-link"
              to={URLS.WORKSPACES}
            >
              <Icon className="fas fa-project-diagram" />
              <span>Workspaces</span>
            </Link>
          </Nav>

          <RightCol>
            <Btn
              data-tip="View ohara version"
              data-testid="version-btn"
              onClick={this.handleVersionModalOpen}
            >
              <i className="fas fa-info-circle" />
            </Btn>

            {/* Features not yet finished, don't display them in the UI */}
            {false && (
              <Login
                data-testid="login-state"
                to={isLogin ? URLS.LOGOUT : URLS.LOGIN}
              >
                {isLogin ? 'Log out' : 'Log in'}
              </Login>
            )}
          </RightCol>
        </HeaderWrapper>
      </StyledHeader>
    );
  }
}

export default Header;
