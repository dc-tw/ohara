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
import PropTypes from 'prop-types';
import { map, includes, get, isNull } from 'lodash';

import * as nodeApi from 'api/nodeApi';
import { Table, Checkbox } from './styles';
import { Modal } from 'components/common/Modal';
import { Box } from 'components/common/Layout';

class NodeSelectModal extends React.Component {
  static propTypes = {
    isActive: PropTypes.bool.isRequired,
    onClose: PropTypes.func.isRequired,
    onConfirm: PropTypes.func.isRequired,
    initNodeNames: PropTypes.arrayOf(PropTypes.string),
  };

  static defaultProps = {
    initNodeNames: [],
  };

  headers = ['#', 'NODE NAME', 'PORT'];

  state = {
    initNodeNames: null,
    isLoading: true,
    nodes: [],
  };

  static getDerivedStateFromProps(nextProps, prevState) {
    if (nextProps.initNodeNames !== prevState.initNodeNames) {
      return {
        initNodeNames: nextProps.initNodeNames,
        nodes: map(prevState.nodes, node => ({
          ...node,
          checked: includes(nextProps.initNodeNames, node.name),
        })),
      };
    }
    return null;
  }

  componentDidMount() {
    this.fetchData();
  }

  fetchData = async () => {
    const res = await nodeApi.fetchNodes();
    this.setState(() => ({ isLoading: false }));
    const nodes = get(res, 'data.result', null);
    if (!isNull(nodes)) {
      this.setState({ nodes });
    }
  };

  handleClose = () => {
    this.props.onClose();
    this.resetChecked();
  };

  handleConfirm = () => {
    const { nodes } = this.state;
    const nodeNames = nodes.reduce((results, node) => {
      if (node.checked) {
        results.push(node.name);
      }
      return results;
    }, []);
    this.props.onConfirm(nodeNames);
  };

  handleChecked = ({ target }) => {
    const { value, checked } = target;
    this.setState(state => {
      return {
        nodes: state.nodes.map(node => {
          if (value === node.name) {
            return Object.assign({}, node, {
              checked,
            });
          }
          return node;
        }),
      };
    });
  };

  handleRowClick = value => {
    this.setState(state => {
      return {
        nodes: state.nodes.map(node => {
          if (value === node.name) {
            return Object.assign({}, node, {
              checked: !node.checked,
            });
          }
          return node;
        }),
      };
    });
  };

  resetChecked = () => {
    this.setState(state => {
      return {
        nodes: map(state.nodes, node => ({
          ...node,
          checked: includes(state.initNodeNames, node.name),
        })),
      };
    });
  };

  render() {
    const { nodes } = this.state;
    return (
      <Modal
        title="Add node"
        isActive={this.props.isActive}
        width="400px"
        handleCancel={this.handleClose}
        handleConfirm={this.handleConfirm}
        confirmBtnText="Add"
        isConfirmDisabled={false}
        showActions={false}
      >
        <Box shadow={false}>
          <Table headers={this.headers}>
            {nodes.map(({ name, checked, port }) => (
              <tr key={name} onClick={() => this.handleRowClick(name)}>
                <td>
                  <Checkbox
                    value={name}
                    onChange={this.handleChecked}
                    checked={checked || false}
                    height="auto"
                  />
                </td>
                <td>{name}</td>
                <td>{port}</td>
              </tr>
            ))}
          </Table>
        </Box>
      </Modal>
    );
  }
}

export default NodeSelectModal;
