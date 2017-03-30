const $ = window.$ = require('jquery');
import React from 'react';
import { Button } from 'react-bootstrap';
import TableElement from './TableElement';
import AsyncSelect from '../../components/AsyncSelect';
import Select from 'react-virtualized-select';

const propTypes = {
  queryEditor: React.PropTypes.object.isRequired,
  tables: React.PropTypes.array,
  actions: React.PropTypes.object,
};

const defaultProps = {
  tables: [],
  actions: {},
};

class SqlEditorLeftBar extends React.PureComponent {
  constructor(props) {
    super(props);
    this.state = {
      tableOptions: [],
    };
  }
  componentWillMount() {
  }
  onChange(event) {
    if(!event) {
      return;
    }
    
    this.props.actions.queryEditorSetDb(this.props.queryEditor, event.value);
    this.setState({ tableName: event.label });
  }
  dbMutator(data) {
    const options = data.result.map((db) => ({ value: db.id, label: db.name }));
    this.props.actions.setDatabases(data.result);
    if (data.result.length === 0) {
      this.props.actions.addAlert({
        bsStyle: 'danger',
        msg: "It seems you don't have access to any database",
      });
    }
    return options;
  }
  resetState() {
    this.props.actions.resetState();
  }
  
  closePopover(ref) {
    this.refs[ref].hide();
  }
  render() {
    const shouldShowReset = window.location.search === '?reset=1';
    return (
      <div className="scrollbar-container">
        <div className="clearfix sql-toolbar scrollbar-content">
          <div>
            <AsyncSelect
              dataEndpoint="/bigquerytablemodelview/api/read"
              onChange={this.onChange.bind(this)}
              value={this.props.queryEditor.dbId}
              actions={this.props.actions}
              databaseId={this.props.queryEditor.dbId}
              valueRenderer={(o) => (
                <div>
                  <span className="text-muted">Table:</span> {o.label}
                </div>
              )}
              mutator={this.dbMutator.bind(this)}
              placeholder="Select a table"
            />
          </div>
  
          {shouldShowReset &&
            <Button bsSize="small" bsStyle="danger" onClick={this.resetState.bind(this)}>
              <i className="fa fa-bomb" /> Reset State
            </Button>
          }
        </div>
      </div>
    );
  }
}
SqlEditorLeftBar.propTypes = propTypes;
SqlEditorLeftBar.defaultProps = defaultProps;

export default SqlEditorLeftBar;
