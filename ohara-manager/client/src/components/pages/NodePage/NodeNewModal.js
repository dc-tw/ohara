import React from 'react';
import PropTypes from 'prop-types';
import { Form, Field, FormSpy } from 'react-final-form';
import toastr from 'toastr';
import { get } from 'lodash';

import * as _ from 'utils/commonUtils';
import * as nodeApis from 'apis/nodeApis';
import * as validateApis from 'apis/validateApis';
import { Modal } from 'common/Modal';
import { Box } from 'common/Layout';
import { FormGroup, Label } from 'common/Form';
import * as MESSAGES from 'constants/messages';
import InputField from './InputField';
import validate from './validate';
import * as s from './Styles';

class NodeNewModal extends React.Component {
  static propTypes = {
    isActive: PropTypes.bool.isRequired,
    handleClose: PropTypes.func.isRequired,
    handleConfirm: PropTypes.func.isRequired,
  };

  state = {
    isValidConnection: false,
  };

  handleModalClose = () => {
    this.props.handleClose();
  };

  onSubmit = async (values, form) => {
    const res = await nodeApis.createNode(values);
    const isSuccess = _.get(res, 'data.isSuccess', false);
    if (isSuccess) {
      form.reset();
      toastr.success(MESSAGES.NODE_CREATION_SUCCESS);
      this.props.handleConfirm();
      this.handleModalClose();
    }
  };

  testConnection = async values => {
    const { name, port, user, password } = values;
    const res = await validateApis.validateNode({
      hostname: name,
      port,
      user,
      password,
    });

    const pass = get(res, 'data.result[0].pass', false);
    this.setState({ isValidConnection: pass });
    if (pass) {
      toastr.success(MESSAGES.TEST_SUCCESS);
    }
  };

  render() {
    const { isValidConnection } = this.state;
    return (
      <Form
        onSubmit={this.onSubmit}
        initialValues={{}}
        validate={validate}
        render={({
          handleSubmit,
          form,
          submitting,
          pristine,
          invalid,
          values,
        }) => {
          return (
            <Modal
              title="New Ohara node"
              isActive={this.props.isActive}
              width="320px"
              handleCancel={() => {
                form.reset();
                this.handleModalClose();
              }}
              handleConfirm={handleSubmit}
              confirmBtnText="Save"
              isConfirmDisabled={
                submitting || pristine || invalid || !isValidConnection
              }
              showActions={true}
            >
              <FormSpy
                subscription={{ values: true }}
                onChange={() => {
                  this.setState({ isValidConnection: false });
                }}
              />
              <form onSubmit={handleSubmit}>
                <Box shadow={false}>
                  <FormGroup data-testid="name">
                    <Label>Host name</Label>
                    <Field
                      name="name"
                      component={InputField}
                      width="100%"
                      placeholder="node-00"
                      data-testid="name-input"
                    />
                  </FormGroup>
                  <FormGroup data-testid="port">
                    <Label>Port</Label>
                    <Field
                      name="port"
                      component={InputField}
                      type="number"
                      min={0}
                      max={65535}
                      width="50%"
                      placeholder="1021"
                      data-testid="port-input"
                    />
                  </FormGroup>
                  <FormGroup data-testid="user">
                    <Label>User name</Label>
                    <Field
                      name="user"
                      component={InputField}
                      width="100%"
                      placeholder="admin"
                      data-testid="user-input"
                    />
                  </FormGroup>
                  <FormGroup data-testid="password">
                    <Label>Password</Label>
                    <Field
                      name="password"
                      component={InputField}
                      type="password"
                      width="100%"
                      placeholder="password"
                      data-testid="password-input"
                    />
                  </FormGroup>
                  <FormGroup data-testid="view-topology">
                    <s.TestConnectionBtn
                      text="Test connection"
                      data-testid="test-connection-button"
                      handleClick={e => {
                        e.preventDefault();
                        this.testConnection(values);
                      }}
                    />
                  </FormGroup>
                </Box>
              </form>
            </Modal>
          );
        }}
      />
    );
  }
}

export default NodeNewModal;
